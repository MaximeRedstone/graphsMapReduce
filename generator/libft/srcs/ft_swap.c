/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_swap.c                                          :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/18 14:14:42 by sikpenou          #+#    #+#             */
/*   Updated: 2019/09/30 14:52:54 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_swap(void **a, void **b, int opt)
{
	void	*tmp;

	if (!a || !b)
		return ;
	tmp = 0;
	ft_memmove(tmp, *a, opt);
	ft_memmove(*a, *b, opt);
	ft_memmove(*b, tmp, opt);
}
