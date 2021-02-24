/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_free_lst_elem.c                                 :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/11/03 12:00:23 by sikpenou          #+#    #+#             */
/*   Updated: 2019/11/06 15:21:30 by hehlinge         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lstfree_elem(t_lst **elem, int opt)
{
	if (opt == FREE_BOTH || opt == FREE_CONTENT)
		easyfree((void **)&(*elem)->content);
	if (opt == FREE_BOTH || opt == FREE_LINKS)
		easyfree((void **)elem);
}
