/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_free_tab.c                                      :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2020/01/03 14:44:35 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 14:44:36 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int		ft_free_tab(void **tab, size_t len)
{
	unsigned int	pos;

	if (!tab)
		return (0);
	pos = 0;
	while (pos < len)
	{
		if (tab[pos])
			easyfree((void **)tab[pos]);
		tab[pos] = NULL;
		pos++;
	}
	easyfree((void **)tab);
	tab = NULL;
	return (pos);
}
