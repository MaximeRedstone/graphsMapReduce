/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_realloc.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 15:43:11 by sikpenou          #+#    #+#             */
/*   Updated: 2019/09/26 15:57:28 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int		ft_realloc(void **zone, long curr_size, long to_add)
{
	char	*tmp;

	if (!(tmp = (char *)easymalloc(sizeof(char) * (curr_size + to_add))))
		return (0);
	ft_memcpy(tmp, *zone, curr_size);
	easyfree(zone);
	*zone = tmp;
	return (curr_size + to_add);
}
